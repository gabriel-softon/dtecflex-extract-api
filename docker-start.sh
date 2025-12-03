#!/bin/bash

echo "🚀 Iniciando containers Docker..."
docker-compose up -d

echo ""
echo "⏳ Aguardando serviços iniciarem..."
sleep 5

echo ""
echo "📊 Status dos containers:"
docker-compose ps

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ Sistema iniciado com sucesso!"
echo ""
echo "🌐 Links de acesso:"
echo ""
echo "   📱 Frontend (Angular):"
echo "      → http://localhost:4200"
echo ""
echo "   🔌 API (FastAPI):"
echo "      → http://localhost:7373"
echo ""
echo "   📚 Documentação da API (Swagger):"
echo "      → http://localhost:7373/api/docs"
echo ""
echo "   🔴 Redis:"
echo "      → localhost:6380 (porta externa)"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "💡 Comandos úteis:"
echo "   • Ver logs:           docker-compose logs -f"
echo "   • Parar serviços:     docker-compose down"
echo "   • Reiniciar:          docker-compose restart"
echo "   • Status:             docker-compose ps"
echo ""


